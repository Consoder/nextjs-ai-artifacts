const apiKey = 'AIzaSyAepXbJyt800u3FtiUEs3eD82PhMzT4K9Y';
fetch(`https://generativelanguage.googleapis.com/v1beta/models?key=${apiKey}`)
  .then(res => res.json())
  .then(data => {
    if (data.models) {
      console.log(data.models.map(m => m.name).filter(n => n.includes('gemini')));
    } else {
      console.log(data);
    }
  })
  .catch(console.error);
